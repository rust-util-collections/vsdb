//!
//! Bit-path representation for the Sparse Merkle Tree.
//!
//! A `BitPath` is a sequence of bits (0 or 1) stored as packed bytes
//! (MSB-first within each byte) plus a bit-length.  It is analogous
//! to `Nibbles` in the MPT but operates on individual bits rather
//! than 4-bit nibbles.
//!

use std::{cmp, fmt};

/// Maximum number of bits a path can hold (a full 256-bit key hash).
const MAX_BITS: usize = 256;
const MAX_BYTES: usize = MAX_BITS / 8;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct BitPath {
    /// Packed bits, MSB-first within each byte, stored inline.
    /// All bits at index >= `bit_len` are always zero — including
    /// whole trailing bytes — so equality, hashing, and byte-wise
    /// comparisons never observe garbage.
    data: [u8; MAX_BYTES],
    /// Number of valid bits (invariant: <= MAX_BITS).
    ///
    /// Every construction path is bounded: `from_hash` is exactly 256,
    /// `slice`/`concat` operate on fragments of one 256-bit key path,
    /// and `from_packed` is validated by its callers.
    bit_len: usize,
}

impl Default for BitPath {
    fn default() -> Self {
        Self {
            data: [0u8; MAX_BYTES],
            bit_len: 0,
        }
    }
}

impl BitPath {
    /// Creates a 256-bit path from a 32-byte hash.
    pub fn from_hash(hash: &[u8; 32]) -> Self {
        Self {
            data: *hash,
            bit_len: MAX_BITS,
        }
    }

    /// Creates a path from raw packed bytes and a bit length.
    ///
    /// `bit_len` must be at most 256 and `data` must hold exactly
    /// `bit_len.div_ceil(8)` bytes (callers deserializing untrusted
    /// input must validate first).  Trailing bits beyond `bit_len` in
    /// the last byte are normalized to zero.
    pub fn from_packed(data: &[u8], bit_len: usize) -> Self {
        debug_assert!(bit_len <= MAX_BITS);
        debug_assert_eq!(data.len(), bit_len.div_ceil(8));
        let mut buf = [0u8; MAX_BYTES];
        let byte_len = cmp::min(data.len(), MAX_BYTES);
        buf[..byte_len].copy_from_slice(&data[..byte_len]);
        let rem = bit_len % 8;
        if rem != 0 && byte_len > 0 {
            // MSB-first packing: keep the high `rem` bits, zero the rest.
            buf[bit_len / 8] &= 0xFFu8 << (8 - rem);
        }
        Self { data: buf, bit_len }
    }

    /// Creates a path from an unpacked slice where each byte is 0 or 1.
    pub fn from_bits(bits: &[u8]) -> Self {
        debug_assert!(bits.len() <= MAX_BITS);
        debug_assert!(bits.iter().all(|&b| b < 2));
        let bit_len = bits.len();
        let mut data = [0u8; MAX_BYTES];
        for (i, &bit) in bits.iter().enumerate() {
            if bit == 1 {
                data[i / 8] |= 0x80 >> (i % 8);
            }
        }
        Self { data, bit_len }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.bit_len
    }

    #[inline]
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.bit_len == 0
    }

    /// Returns the bit at the given index (0 or 1).
    #[inline]
    pub fn bit_at(&self, index: usize) -> u8 {
        debug_assert!(index < self.bit_len);
        (self.data[index / 8] >> (7 - (index % 8))) & 1
    }

    /// Returns the number of leading bits shared with `other`.
    pub fn common_prefix(&self, other: &BitPath) -> usize {
        let min_bits = cmp::min(self.bit_len, other.bit_len);
        let mut matched = 0;

        for i in 0..min_bits.div_ceil(8) {
            let xor = self.data[i] ^ other.data[i];
            if xor == 0 {
                matched += 8;
                if matched >= min_bits {
                    return min_bits;
                }
            } else {
                matched += xor.leading_zeros() as usize;
                return cmp::min(matched, min_bits);
            }
        }
        min_bits
    }

    /// Returns true if this path starts with `prefix`.
    #[cfg(test)]
    pub fn starts_with(&self, prefix: &BitPath) -> bool {
        if self.bit_len < prefix.bit_len {
            return false;
        }
        self.common_prefix(prefix) >= prefix.bit_len
    }

    /// Returns the number of leading bits of `other` that match `self`
    /// starting at bit `offset` — semantically identical to
    /// `self.slice(offset, self.len()).common_prefix(other)` but without
    /// materializing the slice (no allocation, byte-wise compare).
    pub fn common_prefix_from(&self, offset: usize, other: &BitPath) -> usize {
        debug_assert!(offset <= self.bit_len);
        let limit = cmp::min(self.bit_len - offset, other.bit_len);
        if limit == 0 {
            return 0;
        }

        let byte_off = offset / 8;
        let shift = offset % 8;
        let mut matched = 0usize;

        for (j, &b) in other.data.iter().enumerate().take(limit.div_ceil(8)) {
            // Assemble the self byte spanning bits [offset+8j, offset+8j+8).
            let hi = self.data.get(byte_off + j).copied().unwrap_or(0);
            let a = if shift == 0 {
                hi
            } else {
                let lo = self.data.get(byte_off + j + 1).copied().unwrap_or(0);
                (hi << shift) | (lo >> (8 - shift))
            };
            let xor = a ^ b;
            if xor == 0 {
                matched += 8;
                if matched >= limit {
                    return limit;
                }
            } else {
                matched += xor.leading_zeros() as usize;
                return cmp::min(matched, limit);
            }
        }
        limit
    }

    /// Returns true if `self`, viewed from bit `offset`, starts with
    /// `prefix` — the allocation-free equivalent of
    /// `self.slice(offset, self.len()).starts_with(prefix)`.
    pub fn starts_with_from(&self, offset: usize, prefix: &BitPath) -> bool {
        debug_assert!(offset <= self.bit_len);
        self.bit_len - offset >= prefix.bit_len
            && self.common_prefix_from(offset, prefix) >= prefix.bit_len
    }

    /// Extracts bits [start..end) as a new BitPath (byte-wise shifts).
    pub fn slice(&self, start: usize, end: usize) -> BitPath {
        debug_assert!(start <= end && end <= self.bit_len);
        let new_len = end - start;
        if new_len == 0 {
            return BitPath::default();
        }

        let mut data = [0u8; MAX_BYTES];
        let byte_len = new_len.div_ceil(8);
        let s = start / 8;
        let r = start % 8;

        if r == 0 {
            data[..byte_len].copy_from_slice(&self.data[s..s + byte_len]);
        } else {
            for (j, out) in data.iter_mut().enumerate().take(byte_len) {
                let hi = self.data[s + j] << r;
                let lo = self.data.get(s + j + 1).map(|b| b >> (8 - r)).unwrap_or(0);
                *out = hi | lo;
            }
        }

        // Zero any bits copied in beyond the new length.
        let rem = new_len % 8;
        if rem != 0 {
            data[new_len / 8] &= 0xFFu8 << (8 - rem);
        }

        BitPath {
            data,
            bit_len: new_len,
        }
    }

    /// Concatenates `self` and `other` (byte-wise shifts).
    ///
    /// The combined length must stay within 256 bits — guaranteed for
    /// all callers, which only ever reassemble fragments of a single
    /// 256-bit key path.
    pub fn concat(&self, other: &BitPath) -> BitPath {
        let total = self.bit_len + other.bit_len;
        debug_assert!(total <= MAX_BITS);

        // Bits beyond bit_len are always zero, so OR-merging is safe.
        let mut data = self.data;
        let s = self.bit_len / 8;
        let r = self.bit_len % 8;
        let other_bytes = other.bit_len.div_ceil(8);

        if r == 0 {
            data[s..s + other_bytes].copy_from_slice(&other.data[..other_bytes]);
        } else {
            for (j, &b) in other.data.iter().enumerate().take(other_bytes) {
                data[s + j] |= b >> r;
                if let Some(next) = data.get_mut(s + j + 1) {
                    *next |= b << (8 - r);
                }
            }
        }

        BitPath {
            data,
            bit_len: total,
        }
    }

    /// Returns the packed byte representation (exactly
    /// `len().div_ceil(8)` bytes).
    pub fn as_packed(&self) -> &[u8] {
        &self.data[..self.bit_len.div_ceil(8)]
    }
}

impl fmt::Debug for BitPath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BitPath({}:", self.bit_len)?;
        for i in 0..cmp::min(self.bit_len, 32) {
            write!(f, "{}", self.bit_at(i))?;
        }
        if self.bit_len > 32 {
            write!(f, "...")?;
        }
        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_common_prefix_from_matches_slice() {
        let hash: [u8; 32] = [
            0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
            0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10, 0x00, 0xFF, 0x55, 0xAA,
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        ];
        let full = BitPath::from_hash(&hash);

        // Cross-check the offset-based primitives against the
        // slice-materializing reference on a spread of offsets and
        // probe shapes (aligned, unaligned, empty, diverging).
        for offset in [0usize, 1, 3, 7, 8, 9, 15, 64, 100, 200, 250, 255, 256] {
            let reference = full.slice(offset, 256);

            for take in [0usize, 1, 5, 8, 13, 32, 64] {
                let take = cmp::min(take, 256 - offset);
                let probe = reference.slice(0, take);
                assert_eq!(
                    full.common_prefix_from(offset, &probe),
                    reference.common_prefix(&probe),
                    "offset {offset} take {take}"
                );
                assert!(full.starts_with_from(offset, &probe));

                // Flip the last bit of a non-empty probe: must diverge
                // exactly where the reference diverges.
                if take > 0 {
                    let mut bits: Vec<u8> = (0..take).map(|i| probe.bit_at(i)).collect();
                    let last = bits.len() - 1;
                    bits[last] ^= 1;
                    let flipped = BitPath::from_bits(&bits);
                    assert_eq!(
                        full.common_prefix_from(offset, &flipped),
                        reference.common_prefix(&flipped),
                        "flipped offset {offset} take {take}"
                    );
                    assert!(!full.starts_with_from(offset, &flipped));
                }
            }

            // A probe longer than the remaining bits can never be a prefix.
            if offset > 0 {
                let too_long = full.slice(0, 256 - offset + 1);
                assert!(!full.starts_with_from(offset, &too_long));
            }
        }
    }

    #[test]
    fn test_from_hash() {
        let mut hash = [0u8; 32];
        hash[0] = 0b10110000;
        let path = BitPath::from_hash(&hash);
        assert_eq!(path.len(), 256);
        assert_eq!(path.bit_at(0), 1);
        assert_eq!(path.bit_at(1), 0);
        assert_eq!(path.bit_at(2), 1);
        assert_eq!(path.bit_at(3), 1);
        assert_eq!(path.bit_at(4), 0);
    }

    #[test]
    fn test_from_bits() {
        let bits = [1, 0, 1, 1, 0, 0, 1, 0, 1];
        let path = BitPath::from_bits(&bits);
        assert_eq!(path.len(), 9);
        for (i, &expected) in bits.iter().enumerate() {
            assert_eq!(path.bit_at(i), expected, "bit {} mismatch", i);
        }
    }

    #[test]
    fn test_common_prefix() {
        let a = BitPath::from_bits(&[1, 0, 1, 1, 0]);
        let b = BitPath::from_bits(&[1, 0, 1, 0, 1]);
        assert_eq!(a.common_prefix(&b), 3);

        let c = BitPath::from_bits(&[1, 0, 1, 1, 0]);
        assert_eq!(a.common_prefix(&c), 5);

        let d = BitPath::from_bits(&[0]);
        assert_eq!(a.common_prefix(&d), 0);
    }

    #[test]
    fn test_concat() {
        let a = BitPath::from_bits(&[1, 0, 1]);
        let b = BitPath::from_bits(&[0, 1]);
        let c = a.concat(&b);
        assert_eq!(c.len(), 5);
        assert_eq!(c.bit_at(0), 1);
        assert_eq!(c.bit_at(1), 0);
        assert_eq!(c.bit_at(2), 1);
        assert_eq!(c.bit_at(3), 0);
        assert_eq!(c.bit_at(4), 1);
    }

    #[test]
    fn test_starts_with() {
        let path = BitPath::from_bits(&[1, 0, 1, 1, 0]);
        let prefix = BitPath::from_bits(&[1, 0, 1]);
        assert!(path.starts_with(&prefix));
        assert!(!prefix.starts_with(&path));
        assert!(path.starts_with(&BitPath::default()));
    }

    #[test]
    fn test_empty_operations() {
        let empty = BitPath::default();
        assert_eq!(empty.len(), 0);
        assert!(empty.is_empty());

        let path = BitPath::from_bits(&[1, 0]);
        assert_eq!(empty.concat(&path), path);
        assert_eq!(path.concat(&empty), path);
        assert_eq!(empty.common_prefix(&path), 0);
    }

    #[test]
    fn test_full_256_bit_roundtrip() {
        let hash: [u8; 32] = [
            0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF,
            0xFE, 0xDC, 0xBA, 0x98, 0x76, 0x54, 0x32, 0x10, 0x00, 0xFF, 0x55, 0xAA,
            0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        ];
        let path = BitPath::from_hash(&hash);
        let a = path.slice(0, 128);
        let b = path.slice(128, 256);
        let rejoined = a.concat(&b);
        assert_eq!(rejoined.len(), 256);
        for i in 0..256 {
            assert_eq!(
                rejoined.bit_at(i),
                path.bit_at(i),
                "bit {} mismatch after slice+concat",
                i
            );
        }
    }
}
