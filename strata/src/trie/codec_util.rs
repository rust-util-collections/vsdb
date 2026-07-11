//!
//! Shared low-level codec primitives for the MPT and SMT disk caches.
//!
//! Byte-for-byte identical to the helpers formerly duplicated in
//! [`crate::trie::cache`] (MPT) and `crate::trie::smt::cache` — both cache
//! formats rely on these exact encodings, so any change here is a cache
//! format change for **both** tries.
//!

use crate::trie::error::{Result, TrieError};
use sha3::{Digest, Keccak256};
use std::{io::Read, path::Path};

/// Truncated-Keccak checksum length shared by both cache formats.
pub(crate) const CHECKSUM_LEN: usize = 8;
/// Disposable caches above this size are rejected before allocation and
/// rebuilt from authoritative data.
pub(crate) const MAX_CACHE_FILE_BYTES: u64 = 512 * 1024 * 1024;

pub(crate) fn write_varint(buf: &mut Vec<u8>, mut n: usize) {
    while n >= 0x80 {
        buf.push(((n as u8) & 0x7F) | 0x80);
        n >>= 7;
    }
    buf.push(n as u8);
}

pub(crate) fn read_varint(data: &[u8], cursor: &mut usize) -> Result<usize> {
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
        let low = (b & 0x7F) as usize;
        if low > (usize::MAX >> shift) {
            return Err(TrieError::InvalidState("varint overflow".into()));
        }
        let val = low << shift;
        n = n
            .checked_add(val)
            .ok_or_else(|| TrieError::InvalidState("varint overflow".into()))?;
        if b & 0x80 == 0 {
            if shift > 0 && low == 0 {
                return Err(TrieError::InvalidState(
                    "non-canonical varint encoding".into(),
                ));
            }
            break;
        }
        shift += 7;
    }
    Ok(n)
}

pub(crate) fn write_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    write_varint(buf, bytes.len());
    buf.extend_from_slice(bytes);
}

pub(crate) fn read_bytes(data: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = read_varint(data, cursor)?;
    let end = checked_end(*cursor, len, data.len(), "bytes")?;
    let bytes = data[*cursor..end].to_vec();
    *cursor = end;
    Ok(bytes)
}

pub(crate) fn checked_end(
    cursor: usize,
    len: usize,
    total: usize,
    what: &str,
) -> Result<usize> {
    cursor
        .checked_add(len)
        .filter(|end| *end <= total)
        .ok_or_else(|| TrieError::InvalidState(format!("{what} unexpected EOF")))
}

pub(crate) fn validate_cache_file_size(path: &Path) -> Result<()> {
    let len = std::fs::metadata(path).map_err(io_err)?.len();
    if len > MAX_CACHE_FILE_BYTES {
        return Err(TrieError::InvalidState(format!(
            "cache file is {len} bytes, above the {MAX_CACHE_FILE_BYTES}-byte limit"
        )));
    }
    Ok(())
}

pub(crate) fn read_cache_bytes(r: &mut impl Read) -> Result<Vec<u8>> {
    let mut data = Vec::new();
    r.take(MAX_CACHE_FILE_BYTES + 1)
        .read_to_end(&mut data)
        .map_err(io_err)?;
    if data.len() as u64 > MAX_CACHE_FILE_BYTES {
        return Err(TrieError::InvalidState(format!(
            "cache stream exceeds the {MAX_CACHE_FILE_BYTES}-byte limit"
        )));
    }
    Ok(data)
}

pub(crate) fn read_u8(data: &[u8], cursor: &mut usize) -> Result<u8> {
    if *cursor >= data.len() {
        return Err(TrieError::InvalidState("unexpected EOF".into()));
    }
    let v = data[*cursor];
    *cursor += 1;
    Ok(v)
}

pub(crate) fn io_err(e: std::io::Error) -> TrieError {
    TrieError::InvalidState(format!("I/O error: {e}"))
}

/// Computes a truncated Keccak256 checksum (first 8 bytes).
pub(crate) fn compute_checksum(data: &[u8]) -> [u8; CHECKSUM_LEN] {
    let hash = Keccak256::digest(data);
    let mut out = [0u8; CHECKSUM_LEN];
    out.copy_from_slice(&hash[..CHECKSUM_LEN]);
    out
}
