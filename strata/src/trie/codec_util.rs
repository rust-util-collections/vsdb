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

/// Truncated-Keccak checksum length shared by both cache formats.
pub(crate) const CHECKSUM_LEN: usize = 8;

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

pub(crate) fn write_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    write_varint(buf, bytes.len());
    buf.extend_from_slice(bytes);
}

pub(crate) fn read_bytes(data: &[u8], cursor: &mut usize) -> Result<Vec<u8>> {
    let len = read_varint(data, cursor)?;
    if *cursor + len > data.len() {
        return Err(TrieError::InvalidState("bytes unexpected EOF".into()));
    }
    let bytes = data[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(bytes)
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
