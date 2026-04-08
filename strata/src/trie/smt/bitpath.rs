//!
//! Bit-path representation for the Sparse Merkle Tree.
//!
//! A `BitPath` is a sequence of bits (0 or 1) stored as packed bytes
//! (MSB-first within each byte) plus a bit-length.  It is analogous
//! to `Nibbles` in the MPT but operates on individual bits rather
//! than 4-bit nibbles.
//!

use std::cmp;
use std::fmt;

#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct BitPath {
    /// Packed bits, MSB-first within each byte.
    /// Trailing bits in the last byte (beyond `bit_len`) are always zero.
    data: Vec<u8>,
    /// Number of valid bits.
    bit_len: usize,
}

impl BitPath {
    /// Creates a 256-bit path from a 32-byte hash.
    pub fn from_hash(hash: &[u8; 32]) -> Self {
        Self {
            data: hash.to_vec(),
            bit_len: 256,
        }
    }

    /// Creates a path from raw packed bytes and a bit length.
    ///
    /// The caller must ensure that any trailing bits beyond `bit_len`
    /// in the last byte are zero.
    pub fn from_packed(data: Vec<u8>, bit_len: usize) -> Self {
        debug_assert!(
            data.len() == bit_len.div_ceil(8) || (bit_len == 0 && data.is_empty())
        );
        Self { data, bit_len }
    }

    /// Creates a path from an unpacked slice where each byte is 0 or 1.
    pub fn from_bits(bits: &[u8]) -> Self {
        debug_assert!(bits.iter().all(|&b| b < 2));
        let bit_len = bits.len();
        let byte_len = bit_len.div_ceil(8);
        let mut data = vec![0u8; byte_len];
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
        let min_bytes = cmp::min(self.data.len(), other.data.len());
        let min_bits = cmp::min(self.bit_len, other.bit_len);
        let mut matched = 0;

        for i in 0..min_bytes {
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
    pub fn starts_with(&self, prefix: &BitPath) -> bool {
        if self.bit_len < prefix.bit_len {
            return false;
        }
        self.common_prefix(prefix) >= prefix.bit_len
    }

    /// Extracts bits [start..end) as a new BitPath.
    pub fn slice(&self, start: usize, end: usize) -> BitPath {
        debug_assert!(start <= end && end <= self.bit_len);
        let new_len = end - start;
        if new_len == 0 {
            return BitPath::default();
        }

        let byte_len = new_len.div_ceil(8);
        let mut data = vec![0u8; byte_len];
        for i in 0..new_len {
            let src_idx = start + i;
            let bit = (self.data[src_idx / 8] >> (7 - (src_idx % 8))) & 1;
            if bit == 1 {
                data[i / 8] |= 0x80 >> (i % 8);
            }
        }
        BitPath {
            data,
            bit_len: new_len,
        }
    }

    /// Concatenates `self` and `other`.
    pub fn concat(&self, other: &BitPath) -> BitPath {
        if self.is_empty() {
            return other.clone();
        }
        if other.is_empty() {
            return self.clone();
        }

        let total = self.bit_len + other.bit_len;
        let byte_len = total.div_ceil(8);
        let mut data = vec![0u8; byte_len];

        // Copy self bits
        for i in 0..self.bit_len {
            let bit = (self.data[i / 8] >> (7 - (i % 8))) & 1;
            if bit == 1 {
                data[i / 8] |= 0x80 >> (i % 8);
            }
        }
        // Copy other bits
        for i in 0..other.bit_len {
            let dst = self.bit_len + i;
            let bit = (other.data[i / 8] >> (7 - (i % 8))) & 1;
            if bit == 1 {
                data[dst / 8] |= 0x80 >> (dst % 8);
            }
        }

        BitPath {
            data,
            bit_len: total,
        }
    }

    /// Returns the packed byte representation.
    pub fn as_packed(&self) -> &[u8] {
        &self.data
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
