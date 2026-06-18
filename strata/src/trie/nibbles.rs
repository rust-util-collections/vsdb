use std::fmt;

#[derive(Clone, PartialEq, Eq, Hash, Default)]
pub struct Nibbles {
    // Stored as one nibble per byte (0x00..0x0F) for easier manipulation
    data: Vec<u8>,
}

impl Nibbles {
    /// Converts raw key bytes into a nibble path: each byte `b` yields the
    /// high nibble `b >> 4` followed by the low nibble `b & 0x0F`, so the
    /// resulting path length is `2 * key.len()`.
    ///
    /// Leaf vs. extension nodes are distinguished by the node-type codec
    /// tag, so no Hex-Prefix terminator nibble is appended.
    pub fn from_raw(key: &[u8]) -> Self {
        let mut data = Vec::with_capacity(key.len() * 2);
        for &b in key {
            data.push(b >> 4);
            data.push(b & 0x0F);
        }
        Self { data }
    }

    pub fn from_nibbles_unsafe(nibbles: Vec<u8>) -> Self {
        debug_assert!(nibbles.iter().all(|&n| n < 16));
        Self { data: nibbles }
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn at(&self, index: usize) -> u8 {
        self.data[index]
    }

    pub fn common_prefix(&self, other: &Nibbles) -> usize {
        let len = std::cmp::min(self.len(), other.len());
        let mut i = 0;
        while i < len && self.data[i] == other.data[i] {
            i += 1;
        }
        i
    }

    pub fn starts_with(&self, other: &Nibbles) -> bool {
        if self.len() < other.len() {
            return false;
        }
        &self.data[..other.len()] == other.data.as_slice()
    }

    pub fn split_at(&self, idx: usize) -> (Self, Self) {
        let (a, b) = self.data.split_at(idx);
        (Self { data: a.to_vec() }, Self { data: b.to_vec() })
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
}

impl fmt::Debug for Nibbles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Nibbles(")?;
        for n in &self.data {
            write!(f, "{:x}", n)?;
        }
        write!(f, ")")
    }
}
